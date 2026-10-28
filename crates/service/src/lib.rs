use std::{
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use agent_conductor_domain::{
    Actor, ActorId, ActorRole, ClockReading, Command, CommandKind, CorrelationId, EffectIntentSpec,
    EffectKind, IdempotencyKey, LeaseStatus, Rejection, Task, TaskId, TaskState, apply,
};
use agent_conductor_store::{
    AgentRegistration, AttemptLeaseMutation, AuditAppend, CommitBatch, CommitResult,
    ConsumerStateEffect, EffectIntentAppend, EventDelivery, EventLimits, EventSourceIdentity,
    EventStoreError, EventSubscription, PersistedTask, ProjectRegistration, PublishResult,
    RuntimeInvariantViolation, SemanticEvent, Store, StoreError, TaskMutation, TaskRegistration,
    TaskTransitionAppend,
};
use conductor_protocol::{
    CommandAppliedResponse, ErrorBody, ErrorCode, EventDeliveryResponse, EventPublishedResponse,
    HealthResponse, HealthStatus, InvariantResponse, InvariantViolation, Request, Response,
    SemanticEventResponse, TaskResponse,
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

mod clock;
pub use clock::SystemClock;

pub trait ControlService: Send + Sync + 'static {
    fn handle(&self, request: Request) -> impl Future<Output = Result<Response, ErrorBody>> + Send;
}
pub trait Clock: Send + Sync + 'static {
    fn read(&self, boot_generation: u64) -> ClockReading;
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
        let clock = SystemClock::try_new()
            .map_err(|error| StoreError::Integrity(format!("host clock unavailable: {error}")))?;
        Self::start_with_clock(store, started_at_ms, clock).await
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

    async fn attempt_capability_hash(&self, task: &Task) -> Result<Option<String>, ErrorBody> {
        let Some(attempt) = &task.attempt else {
            return Ok(None);
        };
        self.store
            .agent(&attempt.worker.0)
            .await
            .map_err(store_error)?
            .map(|agent| agent.capability_hash)
            .ok_or_else(|| {
                ErrorBody::new(
                    ErrorCode::InvariantViolation,
                    "attempt worker is not registered",
                )
            })
            .map(Some)
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
        let capability_hash = self.attempt_capability_hash(&applied.task).await?;
        let effects = effect_appends(&applied.task, &command, &applied.effects, clock)?;
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
            attempt: attempt(&applied.task, clock, capability_hash.as_deref())?,
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
                path,
            } => {
                validate_text(&[&id, &path])?;
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let project = trusted_project_registration(id.clone(), &path, now)?;
                let out = Response::ProjectRegistered { id: id.clone() };
                self.persist_creation(
                    actor_id,
                    idempotency_key,
                    &request,
                    Some(project),
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
            Request::SubscribeEvents { subscription } => {
                validate_text(&[
                    &subscription.id,
                    &subscription.agent_id,
                    &subscription.topic_filter,
                ])?;
                let id = subscription.id.clone();
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                self.store
                    .upsert_event_subscription(&EventSubscription {
                        schema_version: subscription.schema_version,
                        id: subscription.id,
                        agent_id: subscription.agent_id,
                        topic_filter: subscription.topic_filter,
                        project_id: subscription.project_id,
                        path_prefix: subscription.path_prefix,
                        echo_source: subscription.echo_source,
                        created_at_ms: now,
                    })
                    .await
                    .map_err(event_store_error)?;
                Ok(Response::EventSubscribed {
                    subscription_id: id,
                })
            }
            Request::PublishEvent { event } => {
                validate_text(&[
                    &event.id,
                    &event.source_agent_id,
                    &event.source_session_id,
                    &event.correlation_id,
                    &event.topic,
                ])?;
                let payload_json = serde_json::to_string(&event.payload).map_err(json_error)?;
                let clock = self.clock.read(self.boot_generation);
                if let Some(task_id) = &event.task_id {
                    let row = self
                        .store
                        .task(task_id)
                        .await
                        .map_err(store_error)?
                        .ok_or_else(not_found)?;
                    validate_task_event(&row, &event, clock)?;
                } else if event.attempt_id.is_some() || event.lease_epoch.is_some() {
                    return Err(ErrorBody::new(
                        ErrorCode::Validation,
                        "attempt identity requires a task-scoped event",
                    ));
                }
                let idempotency_key = event.idempotency_key.unwrap_or_else(|| {
                    EventSourceIdentity {
                        source_agent_id: &event.source_agent_id,
                        source_session_id: &event.source_session_id,
                        normalized_payload: &payload_json,
                    }
                    .idempotency_key()
                });
                let result = self
                    .store
                    .publish_event(
                        &SemanticEvent {
                            schema_version: event.schema_version,
                            id: event.id,
                            idempotency_key,
                            source_agent_id: event.source_agent_id,
                            source_session_id: event.source_session_id,
                            correlation_id: event.correlation_id,
                            causation_id: event.causation_id,
                            generation: event.generation,
                            topic: event.topic,
                            project_id: event.project_id,
                            task_id: event.task_id,
                            attempt_id: event.attempt_id,
                            lease_epoch: event.lease_epoch,
                            path_scope: event.path_scope,
                            payload_json,
                            created_at_ms: clock.wall_unix_ms,
                        },
                        &EventLimits::default(),
                    )
                    .await
                    .map_err(event_store_error)?;
                let response = match result {
                    PublishResult::Published { event_id, fanout } => EventPublishedResponse {
                        event_id,
                        fanout,
                        duplicate: false,
                    },
                    PublishResult::Duplicate { event_id } => EventPublishedResponse {
                        event_id,
                        fanout: 0,
                        duplicate: true,
                    },
                };
                Ok(Response::EventPublished(response))
            }
            Request::ClaimEvent { agent_id } => {
                validate_text(&[&agent_id])?;
                let now = self.clock.read(self.boot_generation).wall_unix_ms;
                let delivery = self
                    .store
                    .claim_event_delivery(&agent_id, now, &EventLimits::default())
                    .await
                    .map_err(event_store_error)?
                    .map(event_delivery_response)
                    .transpose()?
                    .map(Box::new);
                Ok(Response::EventDelivery { delivery })
            }
            Request::AcknowledgeEvent {
                delivery_id,
                consumer_agent_id,
                claim_token,
                effect,
            } => {
                validate_text(&[
                    &delivery_id,
                    &consumer_agent_id,
                    &claim_token,
                    &effect.namespace,
                    &effect.key,
                ])?;
                self.store
                    .acknowledge_event_with_state(
                        &delivery_id,
                        &consumer_agent_id,
                        &claim_token,
                        &ConsumerStateEffect {
                            namespace: effect.namespace,
                            key: effect.key,
                            expected_version: effect.expected_version,
                            value_json: serde_json::to_string(&effect.value).map_err(json_error)?,
                        },
                        self.clock.read(self.boot_generation).wall_unix_ms,
                    )
                    .await
                    .map_err(event_store_error)?;
                Ok(Response::EventAcknowledged { delivery_id })
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
fn attempt(
    task: &Task,
    clock: ClockReading,
    capability_hash: Option<&str>,
) -> Result<Option<AttemptLeaseMutation>, ErrorBody> {
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
                instruction_hashes_json: "[]".into(),
                required_checks_json: serde_json::to_string(&task.required_checks)
                    .map_err(json_error)?,
                capability_hash: capability_hash
                    .ok_or_else(|| {
                        ErrorBody::new(
                            ErrorCode::InvariantViolation,
                            "attempt capability identity is unavailable",
                        )
                    })?
                    .to_owned(),
                ended_at_ms: (a.lease.status == LeaseStatus::Ended).then_some(clock.wall_unix_ms),
            })
        })
        .transpose()
}
fn effect_append(
    task: &Task,
    command: &Command,
    effect: &EffectIntentSpec,
    clock: ClockReading,
) -> Result<EffectIntentAppend, ErrorBody> {
    let kind = match effect.kind {
        EffectKind::WorkerSpawn => "worker.spawn",
    };
    let material = format!(
        "v1\0{}\0{}\0{}\0{}\0{kind}",
        task.id.0, effect.attempt_id.0, effect.lease_epoch, command.correlation_id.0
    );
    Ok(EffectIntentAppend {
        token: format!("v1:{:x}", Sha256::digest(material.as_bytes())),
        task_id: Some(task.id.0.clone()),
        attempt_id: Some(effect.attempt_id.0.clone()),
        lease_epoch: Some(to_i64(effect.lease_epoch)?),
        correlation_id: command.correlation_id.0.clone(),
        effect_kind: kind.into(),
        request_json: serde_json::to_string(effect).map_err(json_error)?,
        created_at_ms: clock.wall_unix_ms,
    })
}

fn effect_appends(
    task: &Task,
    command: &Command,
    effects: &[EffectIntentSpec],
    clock: ClockReading,
) -> Result<Vec<EffectIntentAppend>, ErrorBody> {
    effects
        .iter()
        .map(|effect| effect_append(task, command, effect, clock))
        .collect()
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
        StoreError::VersionConflict { .. }
        | StoreError::IdempotencyCollision
        | StoreError::ProjectIdentityConflict => ErrorCode::Conflict,
        StoreError::Integrity(_) | StoreError::InvalidVersionStep { .. } => {
            ErrorCode::InvariantViolation
        }
        _ => ErrorCode::Internal,
    };
    ErrorBody::new(code, format!("persistent state error: {e}"))
}
#[allow(clippy::needless_pass_by_value)]
fn event_store_error(error: EventStoreError) -> ErrorBody {
    let code = match error {
        EventStoreError::IdempotencyCollision
        | EventStoreError::DeliveryNotLeased
        | EventStoreError::ConsumerStateConflict => ErrorCode::Conflict,
        EventStoreError::UnsupportedSchema(_)
        | EventStoreError::InvalidPayload
        | EventStoreError::PayloadTooLarge
        | EventStoreError::GenerationLimit
        | EventStoreError::InvalidCausalGeneration
        | EventStoreError::AgeLimit
        | EventStoreError::FanoutLimit
        | EventStoreError::RetryLimit
        | EventStoreError::NumericRange => ErrorCode::Validation,
        EventStoreError::Database(_) => ErrorCode::Internal,
    };
    ErrorBody::new(code, format!("event state error: {error}"))
}

fn event_delivery_response(delivery: EventDelivery) -> Result<EventDeliveryResponse, ErrorBody> {
    Ok(EventDeliveryResponse {
        schema_version: delivery.schema_version,
        delivery_id: delivery.delivery_id,
        attempts: delivery.attempts,
        claim_token: delivery.claim_token,
        lease_expires_at_ms: delivery.lease_expires_at_ms,
        event: SemanticEventResponse {
            schema_version: delivery.event.schema_version,
            id: delivery.event.id,
            idempotency_key: delivery.event.idempotency_key,
            source_agent_id: delivery.event.source_agent_id,
            source_session_id: delivery.event.source_session_id,
            correlation_id: delivery.event.correlation_id,
            causation_id: delivery.event.causation_id,
            generation: delivery.event.generation,
            topic: delivery.event.topic,
            project_id: delivery.event.project_id,
            task_id: delivery.event.task_id,
            attempt_id: delivery.event.attempt_id,
            lease_epoch: delivery.event.lease_epoch,
            path_scope: delivery.event.path_scope,
            payload: serde_json::from_str(&delivery.event.payload_json).map_err(json_error)?,
            created_at_ms: delivery.event.created_at_ms,
        },
    })
}

fn validate_task_event(
    row: &PersistedTask,
    event: &conductor_protocol::SemanticEventInput,
    clock: ClockReading,
) -> Result<(), ErrorBody> {
    let task: Task = serde_json::from_str(&row.aggregate_json).map_err(json_error)?;
    let attempt = task.attempt.as_ref().ok_or_else(|| {
        ErrorBody::new(ErrorCode::Conflict, "task has no active attempt")
    })?;
    if event.project_id.as_deref() != Some(row.project_id.as_str())
        || event.attempt_id.as_deref() != Some(attempt.id.0.as_str())
        || event.lease_epoch != Some(attempt.lease.epoch)
        || event.source_agent_id != attempt.worker.0
        || !attempt.lease.accepts_worker_mutation(clock)
    {
        return Err(ErrorBody::new(
            ErrorCode::Conflict,
            "task event lease identity is stale or mismatched",
        ));
    }
    Ok(())
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

fn trusted_project_registration(
    id: String,
    path: &str,
    created_at_ms: i64,
) -> Result<ProjectRegistration, ErrorBody> {
    let requested = Path::new(path);
    if !requested.is_absolute() {
        return Err(ErrorBody::new(
            ErrorCode::Validation,
            "project path must be absolute",
        ));
    }
    let canonical = fs::canonicalize(requested).map_err(|error| {
        ErrorBody::new(
            ErrorCode::Validation,
            format!("project path cannot be resolved: {error}"),
        )
    })?;
    let metadata = fs::metadata(&canonical).map_err(project_path_error)?;
    if !metadata.is_dir() {
        return Err(ErrorBody::new(
            ErrorCode::Validation,
            "project path must resolve to a directory",
        ));
    }
    Ok(ProjectRegistration {
        id,
        canonical_path: path_text(&canonical)?,
        filesystem_identity: metadata_identity(&metadata)?,
        git_common_dir_identity: git_common_directory(&canonical)?
            .map(|common| fs::metadata(common).map_err(project_path_error))
            .transpose()?
            .map(|common| metadata_identity(&common))
            .transpose()?,
        created_at_ms,
    })
}

fn git_common_directory(project: &Path) -> Result<Option<PathBuf>, ErrorBody> {
    let dot_git = project.join(".git");
    if dot_git.is_dir() {
        return fs::canonicalize(dot_git)
            .map(Some)
            .map_err(project_path_error);
    }
    if !dot_git.is_file() {
        return Ok(None);
    }
    let pointer = fs::read_to_string(&dot_git).map_err(project_path_error)?;
    let git_dir = pointer
        .trim()
        .strip_prefix("gitdir:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ErrorBody::new(ErrorCode::Validation, "invalid .git pointer"))?;
    let git_dir =
        fs::canonicalize(resolve_from(project, Path::new(git_dir))).map_err(project_path_error)?;
    let common_pointer = git_dir.join("commondir");
    if !common_pointer.is_file() {
        return Ok(Some(git_dir));
    }
    let common = fs::read_to_string(common_pointer).map_err(project_path_error)?;
    let common = common.trim();
    if common.is_empty() {
        return Err(ErrorBody::new(
            ErrorCode::Validation,
            "empty Git common-directory pointer",
        ));
    }
    fs::canonicalize(resolve_from(&git_dir, Path::new(common)))
        .map(Some)
        .map_err(project_path_error)
}

fn resolve_from(base: &Path, value: &Path) -> PathBuf {
    if value.is_absolute() {
        value.to_owned()
    } else {
        base.join(value)
    }
}

#[cfg(unix)]
#[allow(clippy::unnecessary_wraps)]
fn metadata_identity(metadata: &fs::Metadata) -> Result<String, ErrorBody> {
    Ok(format!("unix:{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn metadata_identity(_metadata: &fs::Metadata) -> Result<String, ErrorBody> {
    Err(ErrorBody::new(
        ErrorCode::InvariantViolation,
        "trusted filesystem identity is unsupported on this host",
    ))
}

fn path_text(path: &Path) -> Result<String, ErrorBody> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| ErrorBody::new(ErrorCode::Validation, "project path is not valid UTF-8"))
}

#[allow(clippy::needless_pass_by_value)]
fn project_path_error(error: std::io::Error) -> ErrorBody {
    ErrorBody::new(
        ErrorCode::Validation,
        format!("project identity cannot be resolved: {error}"),
    )
}

fn to_i64(v: u64) -> Result<i64, ErrorBody> {
    i64::try_from(v)
        .map_err(|_| ErrorBody::new(ErrorCode::InvariantViolation, "numeric range exceeded"))
}
fn hash(v: &str) -> String {
    format!("{:x}", Sha256::digest(v.as_bytes()))
}
#[cfg(test)]
mod effect_tests {
    use super::*;
    use agent_conductor_domain::{AttemptId, EffectIntentSpec, EffectKind};

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn command(correlation_id: &str) -> Command {
        Command {
            actor: Actor {
                id: ActorId("worker-1".into()),
                role: ActorRole::Worker,
            },
            expected_version: 1,
            correlation_id: CorrelationId(correlation_id.into()),
            idempotency_key: IdempotencyKey("claim-1".into()),
            reason: "claim".into(),
            kind: CommandKind::Cancel,
        }
    }

    #[test]
    fn effect_token_is_versioned_stable_and_changes_with_identity() {
        let task = Task::new(TaskId("task-1".into()), vec![]);
        let effect = EffectIntentSpec {
            kind: EffectKind::WorkerSpawn,
            attempt_id: AttemptId("attempt-1".into()),
            lease_epoch: 1,
            worker: ActorId("worker-1".into()),
        };
        let clock = ClockReading {
            boot_generation: 7,
            monotonic_ms: 10,
            wall_unix_ms: 20,
        };

        let first = effect_append(&task, &command("corr-1"), &effect, clock).expect("effect");
        let replay = effect_append(&task, &command("corr-1"), &effect, clock).expect("replay");
        assert_eq!(first.token, replay.token);
        assert_eq!(
            first.token,
            "v1:f77d90fe7237d7c900a18c2766422bca4ba18dc1183d1a4812e3a68307313eb6"
        );

        let changed =
            effect_append(&task, &command("corr-2"), &effect, clock).expect("changed effect");
        assert_ne!(first.token, changed.token);
        assert_eq!(first.effect_kind, "worker.spawn");
        assert_eq!(
            serde_json::from_str::<EffectIntentSpec>(&first.request_json).expect("typed request"),
            effect
        );
    }

    #[test]
    fn project_intake_rejects_relative_missing_and_file_paths() {
        assert!(trusted_project_registration("p".into(), "relative", 1).is_err());
        assert!(
            trusted_project_registration("p".into(), "/definitely/missing/conductor", 1).is_err()
        );
        let directory = tempfile::tempdir().expect("temporary directory");
        let file = directory.path().join("file");
        fs::write(&file, "not a project").expect("write file");
        assert!(trusted_project_registration("p".into(), &file.to_string_lossy(), 1).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn project_aliases_resolve_to_one_trusted_identity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let project = directory.path().join("project");
        fs::create_dir(&project).expect("create project");
        let alias = directory.path().join("alias");
        symlink(&project, &alias).expect("create alias");

        let direct = trusted_project_registration("direct".into(), &project.to_string_lossy(), 1)
            .expect("direct identity");
        let aliased = trusted_project_registration("alias".into(), &alias.to_string_lossy(), 1)
            .expect("alias identity");
        assert_eq!(direct.canonical_path, aliased.canonical_path);
        assert_eq!(direct.filesystem_identity, aliased.filesystem_identity);
    }

    #[test]
    fn worktree_pointer_resolves_to_main_git_identity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let main = directory.path().join("main");
        let common = main.join(".git");
        let worktree_git = common.join("worktrees/w1");
        let worktree = directory.path().join("worktree");
        fs::create_dir_all(&worktree_git).expect("create worktree metadata");
        fs::create_dir(&worktree).expect("create worktree");
        fs::write(worktree_git.join("commondir"), "../..").expect("write common pointer");
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}", worktree_git.display()),
        )
        .expect("write git pointer");

        let main_identity = trusted_project_registration("main".into(), &main.to_string_lossy(), 1)
            .expect("main identity");
        let worktree_identity =
            trusted_project_registration("worktree".into(), &worktree.to_string_lossy(), 1)
                .expect("worktree identity");
        assert_eq!(
            main_identity.git_common_dir_identity,
            worktree_identity.git_common_dir_identity
        );
    }
}
