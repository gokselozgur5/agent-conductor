use std::{
    future::Future,
    sync::{Arc, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use agent_conductor_domain::{
    Actor, ActorId, ActorRole, ClockReading, Command, CommandKind, CorrelationId, IdempotencyKey,
    LeaseStatus, Rejection, Task, TaskId, TaskState, apply,
};
use agent_conductor_store::{
    AgentRegistration, AttemptLeaseMutation, AuditAppend, CommitBatch, CommitResult,
    EffectIntentAppend, PersistedTask, ProjectRegistration, RuntimeInvariantViolation, Store,
    StoreError, TaskMutation, TaskRegistration, TaskTransitionAppend,
};
use conductor_protocol::{
    CommandAppliedResponse, ErrorBody, ErrorCode, HealthResponse, HealthStatus, InvariantResponse,
    InvariantViolation, Request, Response, TaskResponse,
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

pub trait ControlService: Send + Sync + 'static {
    fn handle(&self, request: Request) -> impl Future<Output = Result<Response, ErrorBody>> + Send;
}
pub trait Clock: Send + Sync + 'static {
    fn read(&self, boot_generation: u64) -> ClockReading;
}

#[derive(Debug)]
pub struct SystemClock {
    origin: Instant,
}
impl Default for SystemClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}
impl Clock for SystemClock {
    fn read(&self, boot_generation: u64) -> ClockReading {
        ClockReading {
            boot_generation,
            monotonic_ms: u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
            wall_unix_ms: wall_ms(),
        }
    }
}

#[derive(Clone)]
pub struct PersistentControlService<C = SystemClock> {
    store: Store,
    boot_generation: u64,
    started_at: Instant,
    clock: Arc<C>,
    mutations: Arc<Mutex<()>>,
}

impl PersistentControlService<SystemClock> {
    /// Starts after durable integrity and restart recovery checks.
    /// # Errors
    /// Returns an error when durable startup cannot complete.
    pub async fn start(store: Store, started_at_ms: i64) -> Result<Self, StoreError> {
        Self::start_with_clock(store, started_at_ms, SystemClock::default()).await
    }
}

impl<C: Clock> PersistentControlService<C> {
    /// Starts with an injected authoritative clock.
    /// # Errors
    /// Returns an error when durable startup or recovery cannot complete.
    pub async fn start_with_clock(
        store: Store,
        started_at_ms: i64,
        clock: C,
    ) -> Result<Self, StoreError> {
        let startup = store.start_runtime(started_at_ms).await?;
        let service = Self {
            store,
            boot_generation: startup.boot_generation,
            started_at: Instant::now(),
            clock: Arc::new(clock),
            mutations: Arc::new(Mutex::new(())),
        };
        for lease in startup.recovery_pending {
            let Some(row) = service.store.task(&lease.task_id).await? else {
                continue;
            };
            let task: Task = serde_json::from_str(&row.aggregate_json)
                .map_err(|e| StoreError::Integrity(format!("invalid aggregate: {e}")))?;
            if task
                .attempt
                .as_ref()
                .is_some_and(|a| a.lease.status == LeaseStatus::Active)
            {
                let command = Command {
                    actor: Actor {
                        id: ActorId("conductor-runtime".into()),
                        role: ActorRole::System,
                    },
                    expected_version: task.version,
                    correlation_id: CorrelationId(format!("boot-{}", startup.boot_generation)),
                    idempotency_key: IdempotencyKey(format!(
                        "recovery:{}:{}",
                        startup.boot_generation, task.id.0
                    )),
                    reason: "daemon restart fenced active lease".into(),
                    kind: CommandKind::EnterRecovery,
                };
                service
                    .apply_row(row, command)
                    .await
                    .map_err(|e| StoreError::Integrity(e.message))?;
            }
        }
        Ok(service)
    }

    fn task_response(row: PersistedTask) -> Result<TaskResponse, ErrorBody> {
        Ok(TaskResponse {
            project_id: row.project_id,
            objective: row.objective,
            priority: row.priority,
            authority: serde_json::from_str(&row.authority_json).map_err(json_error)?,
            aggregate: Box::new(serde_json::from_str(&row.aggregate_json).map_err(json_error)?),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_creation(
        &self,
        actor: String,
        key: String,
        request: &Request,
        project: Option<ProjectRegistration>,
        agent: Option<AgentRegistration>,
        new_task: Option<TaskRegistration>,
        outcome: Response,
    ) -> Result<Response, ErrorBody> {
        validate_text(&[&actor, &key])?;
        let request_json = serde_json::to_string(request).map_err(json_error)?;
        let hash = hash(&request_json);
        if let Some(json) = self
            .store
            .command_outcome(&actor, &key, &hash)
            .await
            .map_err(store_error)?
        {
            return serde_json::from_str(&json).map_err(json_error);
        }
        let now = self.clock.read(self.boot_generation).wall_unix_ms;
        let kind = match (&project, &agent, &new_task) {
            (Some(_), _, _) => "project.registered",
            (_, Some(_), _) => "agent.registered",
            _ => "task.created",
        };
        let audit_task_id = new_task.as_ref().map(|task| task.id.clone());
        let batch = CommitBatch {
            actor_id: actor.clone(),
            idempotency_key: key,
            request_hash: hash,
            task: None,
            project,
            agent,
            new_task,
            attempt: None,
            transitions: vec![],
            audit: vec![AuditAppend {
                id: Uuid::now_v7().to_string(),
                task_id: audit_task_id,
                actor_id: actor,
                kind: kind.into(),
                correlation_id: Uuid::now_v7().to_string(),
                payload_json: request_json,
                occurred_at_ms: now,
            }],
            effects: vec![],
            outcome_json: serde_json::to_string(&outcome).map_err(json_error)?,
            committed_at_ms: now,
            expires_at_ms: now.saturating_add(86_400_000),
        };
        match self.store.persist(&batch).await.map_err(store_error)? {
            CommitResult::Applied { .. } => Ok(outcome),
            CommitResult::Replayed { outcome_json } => {
                serde_json::from_str(&outcome_json).map_err(json_error)
            }
        }
    }

    async fn apply_row(
        &self,
        row: PersistedTask,
        command: Command,
    ) -> Result<CommandAppliedResponse, ErrorBody> {
        let request_json =
            serde_json::to_string(&(row.id.as_str(), &command)).map_err(json_error)?;
        let request_hash = hash(&request_json);
        if let Some(json) = self
            .store
            .command_outcome(
                &command.actor.id.0,
                &command.idempotency_key.0,
                &request_hash,
            )
            .await
            .map_err(store_error)?
        {
            let mut value: CommandAppliedResponse =
                serde_json::from_str(&json).map_err(json_error)?;
            value.replayed = true;
            return Ok(value);
        }
        if let CommandKind::Claim { worker, .. } = &command.kind
            && !self
                .store
                .agent_exists(&worker.0)
                .await
                .map_err(store_error)?
        {
            return Err(ErrorBody::new(
                ErrorCode::Validation,
                "claim worker is not registered",
            ));
        }
        let old: Task = serde_json::from_str(&row.aggregate_json).map_err(json_error)?;
        let clock = self.clock.read(self.boot_generation);
        let applied = apply(&old, &command, clock).map_err(rejection_error)?;
        let effects = effects(&applied.task, &command, clock);
        let response = CommandAppliedResponse {
            task: TaskResponse {
                project_id: row.project_id,
                objective: row.objective,
                priority: row.priority,
                authority: serde_json::from_str(&row.authority_json).map_err(json_error)?,
                aggregate: Box::new(applied.task.clone()),
            },
            replayed: false,
            effect_tokens: effects.iter().map(|e| e.token.clone()).collect(),
        };
        let batch = CommitBatch {
            actor_id: command.actor.id.0.clone(),
            idempotency_key: command.idempotency_key.0.clone(),
            request_hash,
            task: Some(TaskMutation {
                task_id: applied.task.id.0.clone(),
                expected_version: to_i64(old.version)?,
                new_version: to_i64(applied.task.version)?,
                state: state(applied.task.state)?,
                aggregate_json: serde_json::to_string(&applied.task).map_err(json_error)?,
                updated_at_ms: clock.wall_unix_ms,
            }),
            project: None,
            agent: None,
            new_task: None,
            attempt: attempt(&applied.task, clock)?,
            transitions: vec![TaskTransitionAppend {
                id: Uuid::now_v7().to_string(),
                task_id: applied.task.id.0.clone(),
                actor_id: command.actor.id.0.clone(),
                prior_state: state(applied.transition.from)?,
                new_state: state(applied.transition.to)?,
                reason: command.reason.clone(),
                correlation_id: command.correlation_id.0.clone(),
                occurred_at_ms: clock.wall_unix_ms,
            }],
            audit: vec![AuditAppend {
                id: Uuid::now_v7().to_string(),
                task_id: Some(applied.task.id.0.clone()),
                actor_id: command.actor.id.0,
                kind: command_kind(&command.kind).into(),
                correlation_id: command.correlation_id.0,
                payload_json: request_json,
                occurred_at_ms: clock.wall_unix_ms,
            }],
            effects,
            outcome_json: serde_json::to_string(&response).map_err(json_error)?,
            committed_at_ms: clock.wall_unix_ms,
            expires_at_ms: clock.wall_unix_ms.saturating_add(86_400_000),
        };
        match self.store.persist(&batch).await.map_err(store_error)? {
            CommitResult::Applied { .. } => Ok(response),
            CommitResult::Replayed { outcome_json } => {
                let mut value: CommandAppliedResponse =
                    serde_json::from_str(&outcome_json).map_err(json_error)?;
                value.replayed = true;
                Ok(value)
            }
        }
    }
}

impl<C: Clock> ControlService for PersistentControlService<C> {
    #[allow(clippy::too_many_lines)]
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        match request.clone() {
            Request::Health => {
                let v = invariants(&self.store).await?;
                Ok(Response::Health(HealthResponse {
                    status: if v.is_empty() {
                        HealthStatus::Ready
                    } else {
                        HealthStatus::Degraded
                    },
                    boot_generation: self.boot_generation,
                    uptime_millis: u64::try_from(self.started_at.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                }))
            }
            Request::CheckInvariants => {
                let v = invariants(&self.store).await?;
                Ok(Response::Invariants(InvariantResponse {
                    healthy: v.is_empty(),
                    violations: v,
                }))
            }
            Request::RegisterProject {
                actor_id,
                idempotency_key,
                id,
                canonical_path,
                filesystem_identity,
                git_common_dir_identity,
            } => {
                validate_text(&[&id, &canonical_path, &filesystem_identity])?;
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let out = Response::ProjectRegistered { id: id.clone() };
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    Some(ProjectRegistration {
                        id,
                        canonical_path,
                        filesystem_identity,
                        git_common_dir_identity,
                        created_at_ms: now,
                    }),
                    None,
                    None,
                    out,
                )
                .await
            }
            Request::RegisterAgent {
                actor_id,
                idempotency_key,
                id,
                adapter_kind,
                protocol_fingerprint,
                capability_hash,
                access_mode,
            } => {
                validate_text(&[&id, &adapter_kind, &protocol_fingerprint, &capability_hash])?;
                if !matches!(access_mode.as_str(), "read_only" | "read_write") {
                    return Err(ErrorBody::new(ErrorCode::Validation, "invalid access_mode"));
                }
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let out = Response::AgentRegistered { id: id.clone() };
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    None,
                    Some(AgentRegistration {
                        id,
                        adapter_kind,
                        protocol_fingerprint,
                        capability_hash,
                        access_mode,
                        created_at_ms: now,
                    }),
                    None,
                    out,
                )
                .await
            }
            Request::CreateTask {
                actor_id,
                idempotency_key,
                id,
                project_id,
                objective,
                priority,
                authority,
                required_checks,
            } => {
                validate_text(&[&id, &project_id, &objective])?;
                let aggregate = Task::new(TaskId(id.clone()), required_checks);
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let row = PersistedTask {
                    id: id.clone(),
                    project_id: project_id.clone(),
                    objective: objective.clone(),
                    priority,
                    authority_json: authority.to_string(),
                    aggregate_json: serde_json::to_string(&aggregate).map_err(json_error)?,
                };
                let out = Response::Task(Self::task_response(row)?);
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    None,
                    None,
                    Some(TaskRegistration {
                        id,
                        project_id,
                        objective,
                        state: "backlog".into(),
                        priority,
                        authority_json: authority.to_string(),
                        aggregate_json: serde_json::to_string(&aggregate).map_err(json_error)?,
                        created_at_ms: now,
                    }),
                    out,
                )
                .await
            }
            Request::GetTask { task_id } => {
                let row = self
                    .store
                    .task(&task_id)
                    .await
                    .map_err(store_error)?
                    .ok_or_else(not_found)?;
                Ok(Response::Task(Self::task_response(row)?))
            }
            Request::ListTasks { project_id } => {
                let rows = self
                    .store
                    .tasks(project_id.as_deref())
                    .await
                    .map_err(store_error)?;
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    out.push(Self::task_response(row)?);
                }
                Ok(Response::Tasks(out))
            }
            Request::ApplyCommand { task_id, command } => {
                let _guard = self.mutations.lock().await;
                let row = self
                    .store
                    .task(&task_id)
                    .await
                    .map_err(store_error)?
                    .ok_or_else(not_found)?;
                if row.id != task_id {
                    return Err(ErrorBody::new(
                        ErrorCode::InvariantViolation,
                        "task identity mismatch",
                    ));
                }
                Ok(Response::CommandApplied(
                    self.apply_row(row, command).await?,
                ))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryControlService {
    generation: u64,
    started: Instant,
    violations: Arc<RwLock<Vec<InvariantViolation>>>,
}
impl InMemoryControlService {
    #[must_use]
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            started: Instant::now(),
            violations: Arc::new(RwLock::new(vec![])),
        }
    }
    /// Replaces fake violations.
    ///
    /// # Panics
    ///
    /// Panics if another test thread poisoned the lock.
    pub fn replace_violations(&self, v: Vec<InvariantViolation>) {
        *self.violations.write().expect("lock") = v;
    }
}
impl ControlService for InMemoryControlService {
    async fn handle(&self, request: Request) -> Result<Response, ErrorBody> {
        let v = self.violations.read().expect("lock").clone();
        match request {
            Request::Health => Ok(Response::Health(HealthResponse {
                status: if v.is_empty() {
                    HealthStatus::Ready
                } else {
                    HealthStatus::Degraded
                },
                boot_generation: self.generation,
                uptime_millis: u64::try_from(self.started.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
            })),
            Request::CheckInvariants => Ok(Response::Invariants(InvariantResponse {
                healthy: v.is_empty(),
                violations: v,
            })),
            _ => Err(ErrorBody::new(ErrorCode::Unavailable, "test fake")),
        }
    }
}

async fn invariants(store: &Store) -> Result<Vec<InvariantViolation>, ErrorBody> {
    store
        .check_runtime_invariants()
        .await
        .map_err(store_error)
        .map(|v| v.into_iter().map(map_violation).collect())
}
fn map_violation(v: RuntimeInvariantViolation) -> InvariantViolation {
    InvariantViolation {
        code: v.code,
        summary: v.summary,
        context: serde_json::from_str(&v.context_json)
            .unwrap_or(serde_json::Value::String(v.context_json)),
    }
}
fn attempt(task: &Task, clock: ClockReading) -> Result<Option<AttemptLeaseMutation>, ErrorBody> {
    task.attempt
        .as_ref()
        .map(|a| {
            let remaining = a
                .lease
                .deadline_monotonic_ms
                .saturating_sub(clock.monotonic_ms);
            Ok(AttemptLeaseMutation {
                attempt_id: a.id.0.clone(),
                task_id: task.id.0.clone(),
                worker_id: a.worker.0.clone(),
                lease_epoch: to_i64(a.lease.epoch)?,
                lease_status: match a.lease.status {
                    LeaseStatus::Active => "active",
                    LeaseStatus::RecoveryPending => "recovery_pending",
                    LeaseStatus::Ended => "released",
                }
                .into(),
                boot_generation: to_i64(a.lease.boot_generation)?,
                remaining_ms: to_i64(remaining)?,
                grace_ms: to_i64(a.lease.recovery_grace_ms)?,
                display_expires_at_ms: clock.wall_unix_ms.saturating_add(to_i64(remaining)?),
                last_heartbeat_at_ms: clock.wall_unix_ms,
                required_checks_json: serde_json::to_string(&task.required_checks)
                    .map_err(json_error)?,
                ended_at_ms: (a.lease.status == LeaseStatus::Ended).then_some(clock.wall_unix_ms),
            })
        })
        .transpose()
}
fn effects(task: &Task, command: &Command, clock: ClockReading) -> Vec<EffectIntentAppend> {
    let CommandKind::Claim { attempt_id, .. } = &command.kind else {
        return vec![];
    };
    let material = format!(
        "v1\0{}\0{}\0{}\0{}\0worker.spawn",
        task.id.0, attempt_id.0, task.last_lease_epoch, command.correlation_id.0
    );
    vec![EffectIntentAppend {
        token: format!("v1:{:x}", Sha256::digest(material.as_bytes())),
        task_id: Some(task.id.0.clone()),
        attempt_id: Some(attempt_id.0.clone()),
        lease_epoch: i64::try_from(task.last_lease_epoch).ok(),
        correlation_id: command.correlation_id.0.clone(),
        effect_kind: "worker.spawn".into(),
        request_json: "{}".into(),
        created_at_ms: clock.wall_unix_ms,
    }]
}
fn state(v: TaskState) -> Result<String, ErrorBody> {
    serde_json::to_value(v)
        .map_err(json_error)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| ErrorBody::new(ErrorCode::Internal, "invalid state"))
}
fn command_kind(v: &CommandKind) -> &'static str {
    match v {
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
#[allow(clippy::needless_pass_by_value)]
fn rejection_error(e: Rejection) -> ErrorBody {
    let code = match e {
        Rejection::VersionConflict { .. } | Rejection::InvalidLease(_) => ErrorCode::Conflict,
        Rejection::VerificationIncomplete(_) => ErrorCode::InvariantViolation,
        _ => ErrorCode::Validation,
    };
    ErrorBody::new(code, e.to_string())
}
#[allow(clippy::needless_pass_by_value)]
fn store_error(e: StoreError) -> ErrorBody {
    let code = match e {
        StoreError::VersionConflict { .. } | StoreError::IdempotencyCollision => {
            ErrorCode::Conflict
        }
        StoreError::Integrity(_) | StoreError::InvalidVersionStep { .. } => {
            ErrorCode::InvariantViolation
        }
        _ => ErrorCode::Internal,
    };
    ErrorBody::new(code, format!("persistent state error: {e}"))
}
#[allow(clippy::needless_pass_by_value)]
fn json_error(e: serde_json::Error) -> ErrorBody {
    ErrorBody::new(ErrorCode::Internal, format!("JSON error: {e}"))
}
fn not_found() -> ErrorBody {
    ErrorBody::new(ErrorCode::Validation, "task not found")
}
fn validate_text(v: &[&str]) -> Result<(), ErrorBody> {
    if v.iter().any(|s| s.trim().is_empty()) {
        Err(ErrorBody::new(
            ErrorCode::Validation,
            "required text is empty",
        ))
    } else {
        Ok(())
    }
}
fn to_i64(v: u64) -> Result<i64, ErrorBody> {
    i64::try_from(v)
        .map_err(|_| ErrorBody::new(ErrorCode::InvariantViolation, "numeric range exceeded"))
}
fn hash(v: &str) -> String {
    format!("{:x}", Sha256::digest(v.as_bytes()))
}
fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
