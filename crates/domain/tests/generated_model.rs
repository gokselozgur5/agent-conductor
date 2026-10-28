use agent_conductor_domain::{
    Actor, ActorId, ActorRole, AttemptId, Blocker, CheckId, ClockReading, Command, CommandKind,
    CorrelationId, IdempotencyKey, LeaseStatus, Rejection, RequiredCheck, SourceIdentity, Task,
    TaskId, TaskState, apply,
};

const LEASE_MS: u64 = 10_000;
const MARGIN_MS: u64 = 100;
const GRACE_MS: u64 = 50;

fn clock(boot_generation: u64, monotonic_ms: u64) -> ClockReading {
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

fn command(task: &Task, actor: Actor, kind: CommandKind, key: &str) -> Command {
    Command {
        actor,
        expected_version: task.version,
        correlation_id: CorrelationId::from("generated-model"),
        idempotency_key: IdempotencyKey::from(key),
        reason: "generated lifecycle check".to_owned(),
        kind,
    }
}

fn required_check() -> RequiredCheck {
    RequiredCheck {
        id: CheckId::from("tests"),
        description: "all tests pass".to_owned(),
    }
}

#[derive(Clone, Copy, Debug)]
enum ModelCommand {
    MarkReady,
    Claim,
    Start,
    BeginVerification,
    Pause,
    Block,
    Resume,
    RequeueExpired,
    Cancel,
}

const MODEL_COMMANDS: [ModelCommand; 9] = [
    ModelCommand::MarkReady,
    ModelCommand::Claim,
    ModelCommand::Start,
    ModelCommand::BeginVerification,
    ModelCommand::Pause,
    ModelCommand::Block,
    ModelCommand::Resume,
    ModelCommand::RequeueExpired,
    ModelCommand::Cancel,
];

#[derive(Clone, Copy, Debug)]
struct ReferenceModel {
    state: TaskState,
    version: u64,
}

impl ReferenceModel {
    fn apply(&mut self, command: ModelCommand) -> bool {
        if self.state.is_terminal() {
            return false;
        }
        let next = match command {
            ModelCommand::MarkReady if self.state == TaskState::Backlog => TaskState::Ready,
            ModelCommand::Claim if self.state == TaskState::Ready => TaskState::Claimed,
            ModelCommand::Start if self.state == TaskState::Claimed => TaskState::Running,
            ModelCommand::BeginVerification if self.state == TaskState::Running => {
                TaskState::Verifying
            }
            ModelCommand::Pause
                if matches!(
                    self.state,
                    TaskState::Ready
                        | TaskState::Claimed
                        | TaskState::Running
                        | TaskState::Verifying
                ) =>
            {
                TaskState::Paused
            }
            ModelCommand::Block
                if matches!(
                    self.state,
                    TaskState::Ready
                        | TaskState::Claimed
                        | TaskState::Running
                        | TaskState::Verifying
                ) =>
            {
                TaskState::Blocked
            }
            ModelCommand::Resume
                if matches!(self.state, TaskState::Paused | TaskState::Blocked) =>
            {
                TaskState::Ready
            }
            ModelCommand::RequeueExpired
                if matches!(
                    self.state,
                    TaskState::Claimed | TaskState::Running | TaskState::Verifying
                ) =>
            {
                TaskState::Ready
            }
            ModelCommand::Cancel => TaskState::Cancelled,
            _ => return false,
        };
        self.state = next;
        self.version += 1;
        true
    }
}

fn concrete_command(
    task: &Task,
    model_command: ModelCommand,
    sequence: usize,
    now: &mut u64,
) -> (Command, ClockReading) {
    *now += 1;
    let worker = task
        .attempt
        .as_ref()
        .map_or_else(|| ActorId::from("worker"), |attempt| attempt.worker.clone());
    let attempt_id = task.attempt.as_ref().map_or_else(
        || AttemptId::from("attempt-missing"),
        |attempt| attempt.id.clone(),
    );
    let epoch = task
        .attempt
        .as_ref()
        .map_or(task.last_lease_epoch, |attempt| attempt.lease.epoch);
    let (actor, kind, at) = match model_command {
        ModelCommand::MarkReady => (
            actor("system", ActorRole::System),
            CommandKind::MarkReady,
            *now,
        ),
        ModelCommand::Claim => (
            actor("worker", ActorRole::Worker),
            CommandKind::Claim {
                attempt_id: AttemptId(format!("attempt-{sequence}")),
                worker: ActorId::from("worker"),
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
                recovery_grace_ms: GRACE_MS,
            },
            *now,
        ),
        ModelCommand::Start => (
            Actor {
                id: worker.clone(),
                role: ActorRole::Worker,
            },
            CommandKind::Start {
                attempt_id,
                lease_epoch: epoch,
            },
            *now,
        ),
        ModelCommand::BeginVerification => (
            Actor {
                id: worker,
                role: ActorRole::Worker,
            },
            CommandKind::BeginVerification {
                attempt_id,
                lease_epoch: epoch,
            },
            *now,
        ),
        ModelCommand::Pause => (actor("system", ActorRole::System), CommandKind::Pause, *now),
        ModelCommand::Block => (
            actor("system", ActorRole::System),
            CommandKind::Block {
                blocker: Blocker {
                    kind: "generated".to_owned(),
                    detail: "generated blocker".to_owned(),
                    resume_when: "generated resume condition".to_owned(),
                },
            },
            *now,
        ),
        ModelCommand::Resume => (
            actor("system", ActorRole::System),
            CommandKind::Resume,
            *now,
        ),
        ModelCommand::RequeueExpired => {
            let at = task.attempt.as_ref().map_or(*now, |attempt| {
                attempt.lease.deadline_monotonic_ms + attempt.lease.recovery_grace_ms
            });
            *now = (*now).max(at);
            (
                actor("system", ActorRole::System),
                CommandKind::RequeueExpired,
                at,
            )
        }
        ModelCommand::Cancel => (
            actor("system", ActorRole::System),
            CommandKind::Cancel,
            *now,
        ),
    };
    let boot = task
        .attempt
        .as_ref()
        .map_or(1, |attempt| attempt.lease.boot_generation);
    (
        command(task, actor, kind, &format!("sequence-{sequence}")),
        clock(boot, at),
    )
}

fn check_sequence(sequence: &[ModelCommand]) {
    let mut task = Task::new(TaskId::from("generated-task"), vec![required_check()]);
    let mut model = ReferenceModel {
        state: TaskState::Backlog,
        version: 0,
    };
    let mut now = 0;

    for (index, operation) in sequence.iter().copied().enumerate() {
        let (actual_command, at) = concrete_command(&task, operation, index, &mut now);
        let actual = apply(&task, &actual_command, at);
        let expected_success = model.apply(operation);
        assert_eq!(
            actual.is_ok(),
            expected_success,
            "model disagreed for sequence {sequence:?} at index {index}: {actual:?}"
        );
        if let Ok(outcome) = actual {
            task = outcome.task;
        }
        assert_eq!(task.state, model.state, "state mismatch for {sequence:?}");
        assert_eq!(
            task.version, model.version,
            "version mismatch for {sequence:?}"
        );
        if task.state.is_terminal() {
            assert_eq!(
                task.attempt.as_ref().map(|attempt| attempt.lease.status),
                task.attempt.as_ref().map(|_| LeaseStatus::Ended),
                "terminal task retained a live lease for {sequence:?}"
            );
        }
    }
}

fn generate_sequences(prefix: &mut Vec<ModelCommand>, remaining: usize, checked: &mut usize) {
    check_sequence(prefix);
    *checked += 1;
    if remaining == 0 {
        return;
    }
    for command in MODEL_COMMANDS {
        prefix.push(command);
        generate_sequences(prefix, remaining - 1, checked);
        prefix.pop();
    }
}

#[test]
fn generated_sequences_match_the_independent_lifecycle_model() {
    let mut checked = 0;
    generate_sequences(&mut Vec::new(), 5, &mut checked);
    assert_eq!(checked, 66_430);
}

#[derive(Clone, Copy, Debug)]
enum EdgeCommand {
    MarkReady,
    Claim,
    Start,
    Heartbeat,
    EnterRecovery,
    RecoverHeartbeat,
    RequeueExpired,
    Pause,
    Block,
    Resume,
    BeginVerification,
    BindVerificationSource,
    RecordCheck,
    RecordVerdict,
    RejectVerification,
    Complete,
    AddRequiredCheck,
    Cancel,
}

const EDGE_COMMANDS: [EdgeCommand; 18] = [
    EdgeCommand::MarkReady,
    EdgeCommand::Claim,
    EdgeCommand::Start,
    EdgeCommand::Heartbeat,
    EdgeCommand::EnterRecovery,
    EdgeCommand::RecoverHeartbeat,
    EdgeCommand::RequeueExpired,
    EdgeCommand::Pause,
    EdgeCommand::Block,
    EdgeCommand::Resume,
    EdgeCommand::BeginVerification,
    EdgeCommand::BindVerificationSource,
    EdgeCommand::RecordCheck,
    EdgeCommand::RecordVerdict,
    EdgeCommand::RejectVerification,
    EdgeCommand::Complete,
    EdgeCommand::AddRequiredCheck,
    EdgeCommand::Cancel,
];

const STATES: [TaskState; 9] = [
    TaskState::Backlog,
    TaskState::Ready,
    TaskState::Claimed,
    TaskState::Running,
    TaskState::Paused,
    TaskState::Verifying,
    TaskState::Blocked,
    TaskState::Completed,
    TaskState::Cancelled,
];

fn edge_is_legal(state: TaskState, edge: EdgeCommand) -> bool {
    if state.is_terminal() {
        return false;
    }
    match edge {
        EdgeCommand::MarkReady => state == TaskState::Backlog,
        EdgeCommand::Claim => state == TaskState::Ready,
        EdgeCommand::Start => state == TaskState::Claimed,
        EdgeCommand::Heartbeat | EdgeCommand::EnterRecovery | EdgeCommand::RequeueExpired => {
            matches!(
                state,
                TaskState::Claimed | TaskState::Running | TaskState::Verifying
            )
        }
        EdgeCommand::RecoverHeartbeat => {
            matches!(
                state,
                TaskState::Claimed | TaskState::Running | TaskState::Verifying
            )
        }
        EdgeCommand::Pause | EdgeCommand::Block => matches!(
            state,
            TaskState::Ready | TaskState::Claimed | TaskState::Running | TaskState::Verifying
        ),
        EdgeCommand::Resume => matches!(state, TaskState::Paused | TaskState::Blocked),
        EdgeCommand::BeginVerification => state == TaskState::Running,
        EdgeCommand::BindVerificationSource
        | EdgeCommand::RecordCheck
        | EdgeCommand::RecordVerdict
        | EdgeCommand::RejectVerification
        | EdgeCommand::Complete => state == TaskState::Verifying,
        EdgeCommand::AddRequiredCheck => matches!(state, TaskState::Backlog | TaskState::Ready),
        EdgeCommand::Cancel => true,
    }
}

fn advance(task: &Task, actor: Actor, kind: CommandKind, now: u64) -> Task {
    apply(
        task,
        &command(task, actor, kind, &format!("fixture-{}", task.version)),
        clock(1, now),
    )
    .unwrap()
    .task
}

#[allow(clippy::too_many_lines)] // The fixture spells out each lifecycle state without sharing production transitions.
fn fixture(state: TaskState) -> Task {
    let mut task = Task::new(TaskId::from("edge-task"), vec![required_check()]);
    if state == TaskState::Backlog {
        return task;
    }
    task = advance(
        &task,
        actor("system", ActorRole::System),
        CommandKind::MarkReady,
        1,
    );
    if state == TaskState::Ready {
        return task;
    }
    if state == TaskState::Paused {
        return advance(
            &task,
            actor("system", ActorRole::System),
            CommandKind::Pause,
            2,
        );
    }
    if state == TaskState::Blocked {
        return advance(
            &task,
            actor("system", ActorRole::System),
            CommandKind::Block {
                blocker: Blocker {
                    kind: "fixture".to_owned(),
                    detail: "fixture blocker".to_owned(),
                    resume_when: "fixture resumes".to_owned(),
                },
            },
            2,
        );
    }
    if state == TaskState::Cancelled {
        return advance(
            &task,
            actor("system", ActorRole::System),
            CommandKind::Cancel,
            2,
        );
    }
    task = advance(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::Claim {
            attempt_id: AttemptId::from("edge-attempt"),
            worker: ActorId::from("worker"),
            lease_duration_ms: LEASE_MS,
            worker_margin_ms: MARGIN_MS,
            recovery_grace_ms: GRACE_MS,
        },
        2,
    );
    if state == TaskState::Claimed {
        return task;
    }
    task = advance(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::Start {
            attempt_id: AttemptId::from("edge-attempt"),
            lease_epoch: 1,
        },
        3,
    );
    if state == TaskState::Running {
        return task;
    }
    task = advance(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::BeginVerification {
            attempt_id: AttemptId::from("edge-attempt"),
            lease_epoch: 1,
        },
        4,
    );
    if state == TaskState::Verifying {
        return task;
    }
    let source = SourceIdentity::from("git-tree:edge");
    task = advance(
        &task,
        actor("system", ActorRole::System),
        CommandKind::BindVerificationSource {
            source: source.clone(),
        },
        5,
    );
    task = advance(
        &task,
        actor("system", ActorRole::System),
        CommandKind::RecordCheck {
            check_id: CheckId::from("tests"),
            passed: true,
            source: source.clone(),
        },
        6,
    );
    task = advance(
        &task,
        actor("verifier", ActorRole::Verifier),
        CommandKind::RecordVerdict {
            accepted: true,
            source: source.clone(),
        },
        7,
    );
    advance(
        &task,
        actor("system", ActorRole::System),
        CommandKind::Complete {
            observed_source: source,
        },
        8,
    )
}

#[allow(clippy::too_many_lines)] // The explicit command table is intentionally auditable in one place.
fn edge_command(mut task: Task, edge: EdgeCommand) -> (Task, Command, ClockReading) {
    let source = SourceIdentity::from("git-tree:edge");
    if task.state == TaskState::Verifying
        && matches!(
            edge,
            EdgeCommand::RecordCheck | EdgeCommand::RecordVerdict | EdgeCommand::Complete
        )
    {
        task = advance(
            &task,
            actor("system", ActorRole::System),
            CommandKind::BindVerificationSource {
                source: source.clone(),
            },
            10,
        );
    }
    if task.state == TaskState::Verifying && matches!(edge, EdgeCommand::Complete) {
        task = advance(
            &task,
            actor("system", ActorRole::System),
            CommandKind::RecordCheck {
                check_id: CheckId::from("tests"),
                passed: true,
                source: source.clone(),
            },
            11,
        );
        task = advance(
            &task,
            actor("verifier", ActorRole::Verifier),
            CommandKind::RecordVerdict {
                accepted: true,
                source: source.clone(),
            },
            12,
        );
    }
    if matches!(edge, EdgeCommand::RecoverHeartbeat)
        && matches!(
            task.state,
            TaskState::Claimed | TaskState::Running | TaskState::Verifying
        )
    {
        task = apply(
            &task,
            &command(
                &task,
                actor("system", ActorRole::System),
                CommandKind::EnterRecovery,
                "edge-enter-recovery",
            ),
            clock(2, 20),
        )
        .unwrap()
        .task;
    }
    let attempt_id = task
        .attempt
        .as_ref()
        .map_or_else(|| AttemptId::from("none"), |attempt| attempt.id.clone());
    let epoch = task
        .attempt
        .as_ref()
        .map_or(0, |attempt| attempt.lease.epoch);
    let worker = task
        .attempt
        .as_ref()
        .map_or_else(|| ActorId::from("worker"), |attempt| attempt.worker.clone());
    let (who, kind, at) = match edge {
        EdgeCommand::MarkReady => (
            actor("system", ActorRole::System),
            CommandKind::MarkReady,
            20,
        ),
        EdgeCommand::Claim => (
            actor("worker", ActorRole::Worker),
            CommandKind::Claim {
                attempt_id: AttemptId::from("edge-new-attempt"),
                worker: ActorId::from("worker"),
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
                recovery_grace_ms: GRACE_MS,
            },
            20,
        ),
        EdgeCommand::Start => (
            Actor {
                id: worker.clone(),
                role: ActorRole::Worker,
            },
            CommandKind::Start {
                attempt_id: attempt_id.clone(),
                lease_epoch: epoch,
            },
            20,
        ),
        EdgeCommand::Heartbeat => (
            Actor {
                id: worker.clone(),
                role: ActorRole::Worker,
            },
            CommandKind::Heartbeat {
                attempt_id: attempt_id.clone(),
                lease_epoch: epoch,
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
            },
            20,
        ),
        EdgeCommand::EnterRecovery => (
            actor("system", ActorRole::System),
            CommandKind::EnterRecovery,
            20,
        ),
        EdgeCommand::RecoverHeartbeat => (
            Actor {
                id: worker.clone(),
                role: ActorRole::Worker,
            },
            CommandKind::RecoverHeartbeat {
                attempt_id: attempt_id.clone(),
                lease_epoch: epoch,
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
            },
            21,
        ),
        EdgeCommand::RequeueExpired => (
            actor("system", ActorRole::System),
            CommandKind::RequeueExpired,
            task.attempt.as_ref().map_or(20, |attempt| {
                attempt.lease.deadline_monotonic_ms + attempt.lease.recovery_grace_ms
            }),
        ),
        EdgeCommand::Pause => (actor("system", ActorRole::System), CommandKind::Pause, 20),
        EdgeCommand::Block => (
            actor("system", ActorRole::System),
            CommandKind::Block {
                blocker: Blocker {
                    kind: "edge".to_owned(),
                    detail: "edge blocker".to_owned(),
                    resume_when: "edge resumes".to_owned(),
                },
            },
            20,
        ),
        EdgeCommand::Resume => (actor("system", ActorRole::System), CommandKind::Resume, 20),
        EdgeCommand::BeginVerification => (
            Actor {
                id: worker,
                role: ActorRole::Worker,
            },
            CommandKind::BeginVerification {
                attempt_id,
                lease_epoch: epoch,
            },
            20,
        ),
        EdgeCommand::BindVerificationSource => (
            actor("system", ActorRole::System),
            CommandKind::BindVerificationSource { source },
            20,
        ),
        EdgeCommand::RecordCheck => (
            actor("system", ActorRole::System),
            CommandKind::RecordCheck {
                check_id: CheckId::from("tests"),
                passed: true,
                source,
            },
            20,
        ),
        EdgeCommand::RecordVerdict => (
            actor("verifier", ActorRole::Verifier),
            CommandKind::RecordVerdict {
                accepted: true,
                source,
            },
            20,
        ),
        EdgeCommand::RejectVerification => (
            actor("verifier", ActorRole::Verifier),
            CommandKind::RejectVerification,
            20,
        ),
        EdgeCommand::Complete => (
            actor("system", ActorRole::System),
            CommandKind::Complete {
                observed_source: source,
            },
            20,
        ),
        EdgeCommand::AddRequiredCheck => (
            actor("operator", ActorRole::Operator),
            CommandKind::AddRequiredCheck {
                check: RequiredCheck {
                    id: CheckId::from("lint"),
                    description: "lint passes".to_owned(),
                },
            },
            20,
        ),
        EdgeCommand::Cancel => (actor("system", ActorRole::System), CommandKind::Cancel, 20),
    };
    let boot = task
        .attempt
        .as_ref()
        .map_or(1, |attempt| attempt.lease.boot_generation);
    let boot = if matches!(edge, EdgeCommand::EnterRecovery) {
        boot + 1
    } else {
        boot
    };
    let request = command(&task, who, kind, "edge-command");
    (task, request, clock(boot, at))
}

#[test]
fn every_supported_command_has_an_explicit_legal_and_illegal_state_table() {
    let mut checked = 0;
    for state in STATES {
        for edge in EDGE_COMMANDS {
            let expected = edge_is_legal(state, edge);
            let (task, request, at) = edge_command(fixture(state), edge);
            let result = apply(&task, &request, at);
            assert_eq!(
                result.is_ok(),
                expected,
                "edge mismatch: state={state:?}, command={edge:?}, result={result:?}"
            );
            if !expected {
                assert!(matches!(result, Err(Rejection::IllegalTransition { .. })));
            }
            checked += 1;
        }
    }
    assert_eq!(checked, STATES.len() * EDGE_COMMANDS.len());
}
